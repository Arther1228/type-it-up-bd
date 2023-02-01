package com.yang.flink.demo.datasource;

import lombok.*;

import javax.persistence.Column;
import javax.persistence.Table;
import java.util.Date;

/**
 * @author admin
 */
@Data
@Table(name = "user_info")
@NoArgsConstructor
public class UserInfo {
    @Column(name = "user_id")
    private Integer userId;

    @Column(name = "user_name")
    private String userName;

    @Column(name = "user_real_name")
    private String userRealName;

    @Column(name = "user_pwd")
    private String userPwd;

    @Column(name = "user_tel")
    private String userTel;

    @Column(name = "user_email")
    private String userEmail;

    @Column(name = "user_status")
    private Integer userStatus;

    @Column(name = "user_create_time")
    private Date userCreateTime;

    @Column(name = "user_update_time")
    private Date userUpdateTime;

}
