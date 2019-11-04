CREATE DATABASE marketing;

-- clickstream table
USE marketing;
CREATE TABLE `clickstream`
(
    `id`            INT NOT NULL AUTO_INCREMENT,
    `device_id`     VARCHAR(100),
    `visit_date`    VARCHAR(100),
    `visit_time`    VARCHAR(100),
    `os`            VARCHAR(100),
    `activity_kind` VARCHAR(100),
    `venture`       CHAR(2),
    `session_id`    VARCHAR(100),
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COLLATE utf8mb4_unicode_ci
    AUTO_INCREMENT 5000000;