package com.zhisheng.libraries.event;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: 刘守纲
 * @Date: 2020/8/20 0:45
 */
@AllArgsConstructor
@Data
public class LoginEvent {
    private String userId;
    private long failTime;
    private String ip;
    private String eventType;
}
