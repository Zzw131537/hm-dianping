package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list; // 通用 ? 泛型
    private Long minTime; // 上一次时间
    private Integer offset; // 偏移量
}
