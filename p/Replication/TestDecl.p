test Replication [main = TestTakeoverDrain]: assert NoAckedLoss, ClaimAdvertiseOnce in { TestTakeoverDrain, Node, Client };
test FenceBootZombie [main = TestFenceBootZombie]: assert NoAckedLoss, ClaimAdvertiseOnce, FencedZombie in { TestFenceBootZombie, Node, Client };
test HeartbeatDetection [main = TestHeartbeatDetection]: assert NoAckedLoss, ClaimAdvertiseOnce in { TestHeartbeatDetection, Node, Client };
