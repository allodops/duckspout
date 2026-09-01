//! Armed-vs-fired fault accounting (§8.3's vacuity discipline).

use std::collections::HashMap;
use std::sync::Mutex;

/// How often a fault point was armed and how often it actually fired.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FaultCount {
    /// Times the fault was armed by a schedule.
    pub armed: u64,
    /// Times the armed fault actually fired.
    pub fired: u64,
}

/// The injector ledger: every fault-injection point reports arming and
/// firing here. A run whose schedule armed faults that never fired is
/// **vacuous** — it exercised nothing and must not count as evidence (§8.3);
/// the judge (§8.4) reads this ledger to convict such runs.
#[derive(Debug, Default)]
pub struct InjectorLedger {
    counts: Mutex<HashMap<String, FaultCount>>,
}

impl InjectorLedger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that the fault point `id` was armed.
    pub fn arm(&self, id: &str) {
        let mut counts = self.counts.lock().expect("ledger lock");
        counts.entry(id.to_owned()).or_default().armed += 1;
    }

    /// Records that the fault point `id` fired.
    pub fn fired(&self, id: &str) {
        let mut counts = self.counts.lock().expect("ledger lock");
        counts.entry(id.to_owned()).or_default().fired += 1;
    }

    /// The counts for one fault point.
    #[must_use]
    pub fn count(&self, id: &str) -> FaultCount {
        self.counts
            .lock()
            .expect("ledger lock")
            .get(id)
            .copied()
            .unwrap_or_default()
    }

    /// Fault points that were armed but never fired — the vacuity verdict's
    /// input (§8.3). Sorted, so reports are deterministic.
    #[must_use]
    pub fn vacuously_armed(&self) -> Vec<String> {
        let counts = self.counts.lock().expect("ledger lock");
        let mut vacuous: Vec<String> = counts
            .iter()
            .filter(|(_, count)| count.armed > 0 && count.fired == 0)
            .map(|(id, _)| id.clone())
            .collect();
        vacuous.sort();
        vacuous
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn armed_but_never_fired_is_vacuous() {
        let ledger = InjectorLedger::new();
        ledger.arm("storage:fsync-fail");
        ledger.arm("net:blackhole:a->b");
        ledger.fired("net:blackhole:a->b");
        assert_eq!(
            ledger.vacuously_armed(),
            vec!["storage:fsync-fail".to_owned()]
        );
        assert_eq!(
            ledger.count("net:blackhole:a->b"),
            FaultCount { armed: 1, fired: 1 }
        );
    }

    #[test]
    fn firing_without_arming_is_not_vacuous_but_is_counted() {
        let ledger = InjectorLedger::new();
        ledger.fired("spontaneous");
        assert!(ledger.vacuously_armed().is_empty());
        assert_eq!(
            ledger.count("spontaneous"),
            FaultCount { armed: 0, fired: 1 }
        );
    }
}
