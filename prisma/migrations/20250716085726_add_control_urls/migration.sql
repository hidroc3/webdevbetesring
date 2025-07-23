BEGIN TRY

BEGIN TRAN;

-- AlterTable
ALTER TABLE [dbo].[sirine] ADD [off_control_state_id] NVARCHAR(1000),
[off_control_url] NVARCHAR(1000),
[on_control_state_id] NVARCHAR(1000),
[on_control_url] NVARCHAR(1000);

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH
