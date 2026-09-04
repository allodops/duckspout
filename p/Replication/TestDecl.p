test Replication [main = TestTakeoverDrain]: assert NoAckedLoss, NoAckedLossLive, ClaimAdvertiseOnce in { TestTakeoverDrain, Node, Client };
test FenceBootZombie [main = TestFenceBootZombie]: assert ClaimAdvertiseOnce, FencedZombie in { TestFenceBootZombie, Node, Client };
test HeartbeatDetection [main = TestHeartbeatDetection]: assert NoAckedLoss, NoAckedLossLive, ClaimAdvertiseOnce in { TestHeartbeatDetection, Node, Client };
test DegradedBoot [main = TestDegradedBoot]: assert NoAckedLoss, NoAckedLossLive, ClaimAdvertiseOnce, FencedZombie, NoOwnershipWhileDegraded in { TestDegradedBoot, Node, Client };
test NewNodeBoot [main = TestNewNodeBoot]: assert ClaimAdvertiseOnce, FencedZombie, NoIdentityWhileWaiting in { TestNewNodeBoot, Node, Client };
test GapFreedom [main = TestGapFreedom]: assert GapFreedom, GapFreedomCoverage, ClaimAdvertiseOnce, FencedZombie in { TestGapFreedom, Node, Client };
