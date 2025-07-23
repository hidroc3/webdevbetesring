import {
  IsOptional,
  IsString,
  IsNumber,
  IsDateString,
  IsBoolean,
} from 'class-validator';

export class CreateVehicleTrackingLogDto {
  @IsOptional() @IsNumber() vehicle_id?: number;
  @IsOptional() @IsString() registration?: string;
  @IsOptional() @IsString() chassis_number?: string;
  @IsOptional() @IsDateString() time?: string;
  @IsOptional() @IsNumber() latitude?: number;
  @IsOptional() @IsNumber() longitude?: number;
  @IsOptional() @IsString() position_description?: string;
  @IsOptional() @IsNumber() speed?: number;
  @IsOptional() @IsNumber() bearing?: number;
  @IsOptional() @IsBoolean() vehicle_status?: boolean;
  @IsOptional() @IsBoolean() idling?: boolean;
  @IsOptional() @IsNumber() odometer?: number;
  @IsOptional() @IsNumber() altitude?: number;
  @IsOptional() @IsNumber() fuel_level?: number;
  @IsOptional() @IsNumber() fuel_percentage?: number;
  @IsOptional() @IsDateString() created_at?: string;
}
