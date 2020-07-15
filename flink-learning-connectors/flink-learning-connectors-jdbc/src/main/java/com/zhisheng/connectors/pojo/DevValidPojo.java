package com.zhisheng.connectors.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DevValidPojo {
    String id;
    String devId;
    String devType;
    String devValid;
    Date validDate;
    String orgId;
    String orgName;
    String unitId;
}
