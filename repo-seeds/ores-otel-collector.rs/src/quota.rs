use std::{collections::HashMap, sync::Mutex, time::Instant};

const TOKEN_SCALE: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaConfig {
    pub rate_per_second: u32,
    pub burst: u32,
    pub max_tenants: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaDecision {
    Allowed,
    RateLimited { retry_after_ms: u64 },
    TenantTableFull,
    Unavailable,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens_scaled: u64,
    last_ms: u64,
}

#[derive(Debug)]
pub struct TenantQuotaTable {
    config: QuotaConfig,
    buckets: HashMap<String, Bucket>,
}

impl TenantQuotaTable {
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            config,
            buckets: HashMap::with_capacity(config.max_tenants.min(1024)),
        }
    }

    pub fn admit_at(&mut self, tenant_id: &str, now_ms: u64) -> QuotaDecision {
        if let Some(bucket) = self.buckets.get_mut(tenant_id) {
            return admit_existing(self.config, bucket, now_ms);
        }
        if self.buckets.len() >= self.config.max_tenants {
            return QuotaDecision::TenantTableFull;
        }

        let capacity = scaled_capacity(self.config.burst);
        self.buckets.insert(
            tenant_id.to_owned(),
            Bucket {
                tokens_scaled: capacity.saturating_sub(TOKEN_SCALE),
                last_ms: now_ms,
            },
        );
        QuotaDecision::Allowed
    }

    pub fn tracked_tenants(&self) -> usize {
        self.buckets.len()
    }
}

fn admit_existing(config: QuotaConfig, bucket: &mut Bucket, now_ms: u64) -> QuotaDecision {
    let elapsed_ms = now_ms.saturating_sub(bucket.last_ms);
    bucket.last_ms = now_ms;
    let replenished = elapsed_ms.saturating_mul(u64::from(config.rate_per_second));
    bucket.tokens_scaled = bucket
        .tokens_scaled
        .saturating_add(replenished)
        .min(scaled_capacity(config.burst));

    if bucket.tokens_scaled >= TOKEN_SCALE {
        bucket.tokens_scaled -= TOKEN_SCALE;
        return QuotaDecision::Allowed;
    }

    let deficit = TOKEN_SCALE - bucket.tokens_scaled;
    let rate = u64::from(config.rate_per_second);
    let retry_after_ms = deficit.saturating_add(rate - 1) / rate;
    QuotaDecision::RateLimited { retry_after_ms }
}

fn scaled_capacity(burst: u32) -> u64 {
    u64::from(burst).saturating_mul(TOKEN_SCALE)
}

#[derive(Debug)]
pub struct TenantQuotaGate {
    origin: Instant,
    table: Mutex<TenantQuotaTable>,
}

impl TenantQuotaGate {
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            origin: Instant::now(),
            table: Mutex::new(TenantQuotaTable::new(config)),
        }
    }

    pub fn admit(&self, tenant_id: &str) -> QuotaDecision {
        let now_ms = self.origin.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        match self.table.lock() {
            Ok(mut table) => table.admit_at(tenant_id, now_ms),
            Err(_) => QuotaDecision::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QuotaConfig {
        QuotaConfig {
            rate_per_second: 2,
            burst: 2,
            max_tenants: 2,
        }
    }

    #[test]
    fn burst_is_consumed_then_refilled_deterministically() {
        let mut table = TenantQuotaTable::new(config());
        assert_eq!(table.admit_at("tenant_a", 0), QuotaDecision::Allowed);
        assert_eq!(table.admit_at("tenant_a", 0), QuotaDecision::Allowed);
        assert_eq!(
            table.admit_at("tenant_a", 0),
            QuotaDecision::RateLimited {
                retry_after_ms: 500
            }
        );
        assert_eq!(table.admit_at("tenant_a", 499), QuotaDecision::RateLimited { retry_after_ms: 1 });
        assert_eq!(table.admit_at("tenant_a", 500), QuotaDecision::Allowed);
    }

    #[test]
    fn tenants_have_independent_buckets() {
        let mut table = TenantQuotaTable::new(config());
        assert_eq!(table.admit_at("tenant_a", 0), QuotaDecision::Allowed);
        assert_eq!(table.admit_at("tenant_a", 0), QuotaDecision::Allowed);
        assert!(matches!(
            table.admit_at("tenant_a", 0),
            QuotaDecision::RateLimited { .. }
        ));
        assert_eq!(table.admit_at("tenant_b", 0), QuotaDecision::Allowed);
    }

    #[test]
    fn tenant_table_capacity_fails_closed() {
        let mut table = TenantQuotaTable::new(config());
        assert_eq!(table.admit_at("tenant_a", 0), QuotaDecision::Allowed);
        assert_eq!(table.admit_at("tenant_b", 0), QuotaDecision::Allowed);
        assert_eq!(table.tracked_tenants(), 2);
        assert_eq!(
            table.admit_at("tenant_c", 0),
            QuotaDecision::TenantTableFull
        );
        assert_eq!(table.tracked_tenants(), 2);
    }

    #[test]
    fn clock_regression_never_mints_tokens() {
        let mut table = TenantQuotaTable::new(config());
        assert_eq!(table.admit_at("tenant_a", 1000), QuotaDecision::Allowed);
        assert_eq!(table.admit_at("tenant_a", 1000), QuotaDecision::Allowed);
        assert!(matches!(
            table.admit_at("tenant_a", 999),
            QuotaDecision::RateLimited { .. }
        ));
    }
}
