-- 删除全部表
drop table if exists data_flow;
drop table if exists data_source;
drop table if exists data_group;
drop table if exists login_token;
drop table if exists message;
drop table if exists user;


-- 用户信息表
drop table if exists user;
create table if not exists user (
  id int not null auto_increment comment '主键id',
  user_name varchar(64) not null unique comment '登录账号',
  user_pwd varchar(128) comment '登录密码',
  name varchar(64) comment '用户昵称',
  primary key (id),
  unique index uqi_idx_username (user_name asc)
) default charset utf8;

-- 登录记录
drop table if exists login_token;
create table login_token (
  id int not null auto_increment,
  user_id int not null,
  token varchar(40) not null,
  expired DATETIME not null,
  status tinyint default 0,
  primary key (id),
  unique index uqi_idx_token (token asc),
  foreign key (user_id) references user(id)
) default charset utf8;


-- 数据源分组表
drop table if exists data_group;
create table if not exists data_group (
  id int not null auto_increment comment '主键id',
  group_name varchar(64) comment '分组名称',
  user_id int comment '关联用户id，指定该分组属于哪个用户',
  primary key (id),
  unique (group_name, user_id),
  foreign key (user_id) references user(id),
  index idx_user_id (user_id asc)
) default charset utf8;


-- 数据源信息表
drop table if exists data_source;
create table if not exists data_source (
  id int not null auto_increment comment '主键id',
  source_name varchar(255) comment '数据源名称',
  source_type varchar(255) comment '数据源类型',
  hdfs_path varchar(255) comment 'HDFS路径',
  hive_table varchar(255) comment 'Hive表名称',
  table_column varchar(255) comment 'Hive表结构，可以用符号拼接辅助记录多个信息',
  user_id int comment '关联用户id，指定该数据源属于哪个用户',
  group_id int comment '关联分组id，指定该数据源属于哪个分组',
  create_date varchar(64) comment '创建时间',
  modify_date varchar(64) comment '最后一次修改时间',
  primary key (id),
  foreign key (user_id) references user(id),
  foreign key (group_id) references data_group(id)
) default charset utf8;


-- 数据流程信息表
drop table if exists data_flow;
create table if not exists data_flow (
  id int not null auto_increment comment '主键id',
  flow_name varchar(255) comment '数据流程名称',
  source_id int comment '数据源id - 运行MR时需要',
  flow_type varchar(255) comment '数据流程类型 - MR/HQL',
  mr_name varchar(255) comment 'MR算法名称',
  hive_sql text comment 'Hive运行语句 - 运行预定义的统计功能以及用户自定义sql时需要，可以直接存储sql语句',
  flow_status varchar(255) comment '流程状态 - {create, running, failed, success}',
  result_table varchar(255) comment '结果表名称 - HQL流程运行完成后进行记录',
  result_path varchar(255) comment '结果路径 - MR流程运行完成后进行记录',
  user_id int comment '关联用户id，指定该数据流程属于哪个用户',
  primary key (id),
#   unique (user_id, flow_name),
  foreign key (user_id) references user(id),
  foreign key (source_id) references data_source(id)
) default charset utf8;


-- 消息通知
drop table if exists message;
create table if not exists  message (
  id int not null auto_increment  comment '主键id',
  user_id int not null comment '消息所有者',
  content text comment '消息内容',
  create_date varchar(64) comment '消息创建时间',
  has_read tinyint comment '0:未读,1:已读',
  primary key (id),
  foreign key (user_id) references user(id)
) default charset utf8;



