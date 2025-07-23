import { IsDateString, IsNumber, IsOptional, IsString } from 'class-validator';

export class CreateAwsStationDto {
  @IsOptional()
  @IsString()
  device_id?: string;

  @IsOptional()
  @IsString()
  managed_by?: string;

  @IsOptional()
  @IsString()
  post_name?: string;

  @IsOptional()
  @IsNumber()
  dasId?: number;

  @IsOptional()
  @IsDateString()
  time?: Date;

  @IsOptional()
  @IsNumber()
  rainfall?: number;

  @IsOptional()
  @IsNumber()
  air_temperature_avg?: number;

  @IsOptional()
  @IsNumber()
  relative_humidity?: number;

  @IsOptional()
  @IsNumber()
  wind_speed?: number;

  @IsOptional()
  @IsNumber()
  wind_direction?: number;

  @IsOptional()
  @IsNumber()
  solar_radiation_avg?: number;

  @IsOptional()
  @IsNumber()
  battery?: number;

  @IsOptional()
  @IsNumber()
  panel_temperature?: number;

  @IsOptional()
  @IsString()
  image?: string;

  @IsOptional()
  @IsNumber()
  villageId?: number;

  @IsOptional()
  @IsNumber()
  latitude?: number;

  @IsOptional()
  @IsNumber()
  longitude?: number;

  @IsOptional()
  @IsString()
  built_by?: string;

  @IsOptional()
  @IsString()
  built_year?: string;

  @IsOptional()
  @IsString()
  renovated_by?: string;

  @IsOptional()
  @IsString()
  renovated_year?: string;

  @IsOptional()
  @IsNumber()
  alert_level_1?: number;

  @IsOptional()
  @IsNumber()
  alert_level_2?: number;

  @IsOptional()
  @IsNumber()
  alert_level_3?: number;

  @IsOptional()
  @IsNumber()
  alert_level_4?: number;

  @IsOptional()
  @IsString()
  status?: string;
}
