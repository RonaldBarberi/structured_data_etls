/*
READ.ME:
	The following stored procedure, written in MySQL Workbench, processes a database with millions of records.
	It performs data cleansing through various exclusions, updates the management status of records based on
	the tools used, and calculates an average transaction rate. This approach enables continued client
	management by targeting lower transaction ratios, thereby extending the database's useful life.
*/

SET @days = input_days;
SET @start_datetime = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL @days DAY), '%Y-%m-%d 00:00:00');
SET @end_datetime = DATE_FORMAT(CURDATE(), '%Y-%m-%d 23:59:59');
SET @periodo = DATE_FORMAT(@start_datetime, '%Y%m');


-- Update exclusion_ventas
WITH cte_ventas AS (
SELECT phone
FROM databasename.tb_bdd_ventas_port
UNION ALL
SELECT phone
FROM databasename.tb_bdd_ventas_mig
UNION ALL
SELECT phone
FROM databasename.tb_bdd_ventas_hog
UNION ALL
SELECT phone
FROM databasename.tb_bdd_ventas_other
)
UPDATE databasename.tb_databasename I
INNER JOIN cte_ventas II
	ON I.phone = II.phone
SET I.exclusion_ventas = 1,
	I.apto = 0,
	I.motivo_no_apto = 'VENTA';

-- Update exclusion_blacklist
UPDATE databasename.tb_databasename I
FORCE INDEX(id_apto)
INNER JOIN (
SELECT blacklist
FROM databasename.tb_blacklist
) II
	ON I.phone = II.blacklist
SET I.exclusion_blacklist = 1,
	I.apto = 0,
    I.motivo_no_apto = 'BLACKLIST'
WHERE I.apto = 1;

-- Update exclusion client pospago 
UPDATE databasename.tb_databasename I
FORCE INDEX(id_apto)
INNER JOIN (
SELECT tele_numb
FROM databasename.tb_pospago
) II
	ON I.phone = II.tele_numb
SET I.exclusion_pospago = 1
WHERE I.apto = 1;


-- Update exlucion_operador
UPDATE databasename.tb_databasename
SET apto = 0,
    motivo_no_apto = 'OPERADOR'
WHERE apto = 1
	AND network NOT IN('Otros Operadores', 'Claro (Comcel - Comunicacion Celular S.A.)');


-- Update exclusion phone
UPDATE databasename.tb_databasename
SET apto = 0,
    motivo_no_apto = 'TELEFONO'
WHERE apto = 1
	AND LEFT(phone, 1) != 3;


-- Update sms
WITH cte_sms AS (
SELECT
	phone,
    SUM(sms) sms,
    SUM(cto_sms) cto_sms,
    SUM(no_cto_sms) no_cto_sms,
    MAX(date_last_sms) date_last_sms,
    `status`,
    network
FROM databasename.tb_tool_sms_one
WHERE date_last_sms BETWEEN @start_datetime AND @end_datetime
GROUP BY 1
UNION ALL
SELECT
	phone,
    SUM(sms) sms,
    SUM(cto_sms) cto_sms,
    SUM(no_cto_sms) no_cto_sms,
    MAX(date_last_sms) date_last_sms,
    `status`,
    'Otros Operadores' network
FROM databasename.tb_tool_sms_two
WHERE date_last_sms BETWEEN @start_datetime AND @end_datetime
GROUP BY 1
UNION ALL
SELECT
	phone,
    SUM(sms) sms,
    SUM(cto_sms) cto_sms,
    SUM(no_cto_sms) no_cto_sms,
    MAX(date_last_sms) date_last_sms,
    `status`,
    network
FROM databasename.tb_tool_sms_three
WHERE date_last_sms BETWEEN @start_datetime AND @end_datetime
GROUP BY 1
)
UPDATE databasename.tb_databasename I
INNER JOIN cte_sms II
	ON I.phone = II.phone
SET I.sms = II.sms,
	I.cto_sms = II.cto_sms,
	I.no_cto_sms = II.no_cto_sms,
	I.date_last_sms = II.date_last_sms,
	I.last_status_sms = II.`status`,
    I.network = II.network;


-- Update ivr
WITH cte_ivr AS (
SELECT
    phone,
    SUM(ivr) ivr,
    SUM(transferred) transferred,
    SUM(no_cto_ivr) no_cto_ivr,
    MAX(date_last_ivr) date_last_ivr,
    MAX(`status`) `status`,
    network
FROM databasename.tb_tool_ivr_one
WHERE date_last_ivr BETWEEN @start_datetime AND @end_datetime
GROUP BY phone
UNION ALL
SELECT
    phone,
    SUM(ivr) ivr,
    SUM(transferred) transferred,
    SUM(no_cto_ivr) no_cto_ivr,
    MAX(date_last_ivr) date_last_ivr,
    MAX(`status`) `status`,
    network
FROM databasename.tb_tool_ivr_two
WHERE date_last_ivr BETWEEN @start_datetime AND @end_datetime
GROUP BY phone
)
UPDATE databasename.tb_databasename I
INNER JOIN cte_ivr II
	ON I.phone = II.phone
SET I.ivr = II.ivr,
	I.cto_ivr = II.transferred,
    I.no_cto_ivr = II.no_cto_ivr,
    I.date_last_ivr = II.date_last_ivr,
    I.date_last_cto_ivr = II.date_last_ivr,
    I.last_status_ivr = `status`,
    I.network = II.network;


-- Update calls
UPDATE databasename.tb_databasename I
INNER JOIN (
SELECT
    plataforma,
    date_last_call,
    MAX(IF(cto_calls >= 1, date_last_call, '0000-00-00 00:00:00')) cto_date_max,
    phone,
    SUM(calls) calls,
    SUM(cto_calls) cto_calls,
    SUM(no_cto_calls) no_cto_calls,
    IFNULL(MAX(`status`),'') status_name
FROM databasename.tb_tool_calls_one
WHERE date_last_call BETWEEN @start_datetime AND @end_datetime
GROUP BY plataforma, phone
) II
	ON I.phone = II.phone
SET I.inb = IF(II.plataforma = 'INBOUND', II.calls, I.inb),
	I.cto_inb = IF(II.plataforma = 'INBOUND', II.cto_calls, I.cto_inb),
    I.no_cto_inb = IF(II.plataforma = 'INBOUND', II.no_cto_calls, I.no_cto_inb),
    I.date_last_inb = IF(II.plataforma = 'INBOUND', II.date_last_call, I.date_last_inb),
    I.date_last_cto_inb = IF(II.plataforma = 'INBOUND', II.cto_date_max, I.date_last_cto_inb),
    I.last_status_inb = IF(II.plataforma = 'INBOUND', II.status_name, I.last_status_inb),
    I.`out` = IF(II.plataforma = 'PREDICTIVO', II.calls, I.`out`),
    I.cto_out = IF(II.plataforma = 'PREDICTIVO', II.cto_calls, I.cto_out),
    I.no_cto_out = IF(II.plataforma = 'PREDICTIVO', II.no_cto_calls, I.no_cto_out),
    I.date_last_out = IF(II.plataforma = 'PREDICTIVO', II.date_last_call, I.date_last_out),
    I.date_last_cto_out = IF(II.plataforma = 'PREDICTIVO', II.cto_date_max, I.date_last_cto_out),
    I.last_status_out = IF(II.plataforma = 'PREDICTIVO', II.status_name, I.last_status_out),
    I.blaster = IF(II.plataforma = 'BLASTER', II.calls, I.blaster),
    I.cto_blaster = IF(II.plataforma = 'BLASTER', II.cto_calls, I.cto_blaster),
    I.no_cto_blaster = IF(II.plataforma = 'BLASTER', II.no_cto_calls, I.no_cto_blaster),
    I.date_last_blaster = IF(II.plataforma = 'BLASTER', II.date_last_call, I.date_last_blaster),
    I.date_last_cto_blaster = IF(II.plataforma = 'BLASTER', II.cto_date_max, I.date_last_cto_blaster),
    I.last_status_blaster = IF(II.plataforma = 'BLASTER', II.status_name, I.last_status_blaster);


-- Update whatsapp
UPDATE databasename.tb_databasename I
INNER JOIN (
SELECT
    RIGHT(MES_FROM, 10) AS phone,
    COUNT(MES_CREATION_DATE) cant_msg,
    SUM(IF(MES_SMS_STATUS = 'received', 1, 0)) AS cto_wpp,
    SUM(IF(MES_SMS_STATUS = 'delivery_failed', 1, 0)) AS no_cto_wpp,
    MAX(MES_CREATION_DATE) AS msg_date,
    MAX(IF(MES_SMS_STATUS = 'received', MES_CREATION_DATE, '0000-00-00 00:00:00')) AS cto_date_max
FROM databasename.tb_tool_whatsapp_one
WHERE MES_CREATION_DATE BETWEEN @start_datetime AND @end_datetime
GROUP BY MES_FROM
) II
	ON I.phone = II.phone
SET I.whatsapp = II.cant_msg,
	I.cto_whatsapp = II.cto_wpp,
    I.no_cto_whatsapp = II.no_cto_wpp,
    I.date_last_whastapp = II.msg_date,
    I.date_last_cto_whastapp = II.cto_date_max;


-- Update transacciones
UPDATE databasename.tb_databasename
SET trx = ivr + inb + blaster + `out` + sms,
	cto_trx = cto_ivr + cto_inb + cto_blaster + cto_out + cto_sms,
	ratio_trx = (ivr + inb + blaster + `out` + cto_sms) / 30,
	ratio_cto_trx = (cto_ivr + cto_inb + cto_blaster + cto_out + cto_sms) / 30,
	no_cto_trx = no_cto_ivr + no_cto_inb + no_cto_blaster + no_cto_out + no_cto_sms,
	date_last_trx = GREATEST(date_last_sms, date_last_ivr, date_last_inb, date_last_out, date_last_blaster),
    date_last_cto_trx = GREATEST(date_last_sms, date_last_cto_ivr, date_last_cto_inb, date_last_cto_out, date_last_cto_blaster);
