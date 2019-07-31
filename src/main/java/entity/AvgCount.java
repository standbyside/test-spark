package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 计算平均值.
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class AvgCount implements Serializable {

  public int total;
  public int num;

}
