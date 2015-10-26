package algorithms.btgcomputation;

/**
 *
 */
public enum BTGVertexType {
  /**
   * Vertices that are created during a business transaction, like
   * invoices, quotations, deliveries.
   */
  TRANSACTIONAL {
    @Override
    public String toString() {
      return "TransData";
    }
  },
  /**
   * Vertices that take part in a business transaction, like users, products,
   * vendors.
   */
  MASTER {
    @Override
    public String toString() {
      return "MasterData";
    }
  }
}
