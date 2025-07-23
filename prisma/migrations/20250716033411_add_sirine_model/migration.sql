BEGIN TRY

BEGIN TRAN;

-- CreateTable
CREATE TABLE [dbo].[sirine] (
    [id] BIGINT NOT NULL IDENTITY(1,1),
    [name] NVARCHAR(1000),
    [address] NVARCHAR(1000),
    [latitude] FLOAT(53),
    [longitude] FLOAT(53),
    [device_name] NVARCHAR(1000),
    [firmware_version] NVARCHAR(1000),
    [status_logger] NVARCHAR(1000),
    [last_comm_success] DATETIME2,
    [is_active] BIT,
    [outdated_alarm_status] NVARCHAR(1000),
    [outdated_data_time] DATETIME2,
    [battery_voltage] FLOAT(53),
    [battery_unit] NVARCHAR(1000),
    [battery_status_alarm] NVARCHAR(1000),
    [on_control_value] INT,
    [on_control_state] NVARCHAR(1000),
    [off_control_value] INT,
    [off_control_state] NVARCHAR(1000),
    CONSTRAINT [sirine_pkey] PRIMARY KEY CLUSTERED ([id])
);

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH
