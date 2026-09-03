test Replication [main = TestTakeoverDrain]: assert NoAckedLoss, ClaimAdvertiseOnce in { TestTakeoverDrain, Node, Client };
test FenceBootZombie [main = TestFenceBootZombie]: assert NoAckedLoss, ClaimAdvertiseOnce, FencedZombie in { TestFenceBootZombie, Node, Client };
test HeartbeatDetection [main = TestHeartbeatDetection]: assert NoAckedLoss, ClaimAdvertiseOnce in { TestHeartbeatDetection, Node, Client };
test DegradedBoot [main = TestDegradedBoot]: assert NoAckedLoss, ClaimAdvertiseOnce, FencedZombie, NoOwnershipWhileDegraded in { TestDegradedBoot, Node, Client };
test NewNodeBoot [main = TestNewNodeBoot]: assert ClaimAdvertiseOnce, FencedZombie, NoIdentityWhileWaiting in { TestNewNodeBoot, Node, Client };
