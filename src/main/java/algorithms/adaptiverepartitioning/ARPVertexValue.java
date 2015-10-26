package algorithms.adaptiverepartitioning;

public class  ARPVertexValue {
  private long currentPartition;
  private long desiredPartition;


  public ARPVertexValue(long currentPartition,
    long desiredPartition) {
    this.currentPartition = currentPartition;
    this.desiredPartition = desiredPartition;
  }

  @Override
  public String toString(){
    return Long.toString(this.currentPartition);
  }

  public long getCurrentPartition() {
    return currentPartition;
  }

  public void setCurrentPartition(long currentPartition) {
    this.currentPartition = currentPartition;
  }

  public long getDesiredPartition() {
    return desiredPartition;
  }

  public void setDesiredPartition(long desiredPartition) {
    this.desiredPartition = desiredPartition;
  }
}
