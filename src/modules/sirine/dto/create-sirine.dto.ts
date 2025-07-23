import {
  IsString,
  IsNumber,
  IsBoolean,
  IsOptional,
  IsDate,
} from 'class-validator';
import { Type } from 'class-transformer';

export class CreateSirineDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  address?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  latitude?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  longitude?: number;

  @IsOptional()
  @IsString()
  device_name?: string;

  @IsOptional()
  @IsString()
  firmware_version?: string;

  @IsOptional()
  @IsString()
  status_logger?: string;

  @IsOptional()
  @Type(() => Date)
  @IsDate()
  last_comm_success?: Date;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;

  @IsOptional()
  @IsString()
  outdated_alarm_status?: string;

  @IsOptional()
  @Type(() => Date)
  @IsDate()
  outdated_data_time?: Date;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  battery_voltage?: number;

  @IsOptional()
  @IsString()
  battery_unit?: string;

  @IsOptional()
  @IsString()
  battery_status_alarm?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  on_control_value?: number;

  @IsOptional()
  @IsString()
  on_control_state?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  off_control_value?: number;

  @IsOptional()
  @IsString()
  off_control_state?: string;

  // ✅ Tambahan
  @IsOptional()
  @IsString()
  on_control_url?: string;

  @IsOptional()
  @IsString()
  on_control_state_id?: string;

  @IsOptional()
  @IsString()
  off_control_url?: string;

  @IsOptional()
  @IsString()
  off_control_state_id?: string;

  // ✅ Tambahan baru untuk perbaikan error
  @IsOptional()
  @IsBoolean()
  is_sirine_on?: boolean;
}
