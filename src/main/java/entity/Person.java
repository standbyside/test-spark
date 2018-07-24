package entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaona
 * @create 2018/7/26 下午4:24
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable {

  private String name;
  private Integer age;

}
