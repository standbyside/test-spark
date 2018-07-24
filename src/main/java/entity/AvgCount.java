package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 计算平均值.
 *
 * @author zhaona
 * @create 2018/7/24 下午7:35
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class AvgCount implements Serializable {

  public int total;
  public int num;

}
