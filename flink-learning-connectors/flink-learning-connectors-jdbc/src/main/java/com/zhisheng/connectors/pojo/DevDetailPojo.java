package com.zhisheng.connectors.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * @Author: 刘守纲
 * @Date: 2020/7/14 16:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DevDetailPojo implements Serializable {
    static final long serialVersionUID = 1L;

    private String id;
    private String terminalId;
    private String transformerId;
    private String devType;
    private String devValid;
    private Date validDate;
    private Date actionDate;
    private String unitId;
    private long dataType;
    private String orgId;
    private String orgName;
    private long times;
}
