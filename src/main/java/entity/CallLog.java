package entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 呼叫日志.
 *
 * @author zhaona
 * @create 2018/7/26 下午4:24
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallLog implements Serializable {

  public String callSign;
  public Double contactLat;
  public Double contactLong;
  public Double myLat;
  public Double myLong;

}
