machine Client {
  start state Init {
    entry (input: (owner: Node, key: int, sq: int)) {
      send input.owner, eWriteReq, (client = this, key = input.key, sq = input.sq);
      goto WaitAck;
    }
  }

  state WaitAck {
    on eWriteAck do (ack: tWriteAck) {
      print format ("client: got ack for key {0}", ack.key);
    }
  }
}
