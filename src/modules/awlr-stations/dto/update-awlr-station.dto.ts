import { IsOptional, IsString, IsNumber, IsDate } from 'class-validator';
import { Type } from 'class-transformer';

export class UpdateAwlrStationDto {
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
  @Type(() => Number)
  dasId?: number;

  @IsOptional()
  @IsString()
  river_name?: string;

  @IsOptional()
  @Type(() => Date)
  @IsDate()
  time?: Date;

  @IsOptional()
  @Type(() => Number)
  water_level?: number;

  @IsOptional()
  @Type(() => Number)
  battery?: number;

  @IsOptional()
  @Type(() => Number)
  panel_temperature?: number;

  @IsOptional()
  @IsString()
  image?: string;

  @IsOptional()
  @Type(() => Number)
  flow_ho?: number;

  @IsOptional()
  @Type(() => Number)
  flow_a?: number;

  @IsOptional()
  @Type(() => Number)
  flow_b?: number;

  @IsOptional()
  @Type(() => Number)
  debit?: number;

  @IsOptional()
  @Type(() => Number)
  villageId?: number;

  @IsOptional()
  @Type(() => Number)
  latitude?: number;

  @IsOptional()
  @Type(() => Number)
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
  @Type(() => Number)
  alert_level_1?: number;

  @IsOptional()
  @Type(() => Number)
  alert_level_2?: number;

  @IsOptional()
  @Type(() => Number)
  alert_level_3?: number;

  @IsOptional()
  @Type(() => Number)
  alert_level_4?: number;

  @IsOptional()
  @IsString()
  status?: string;
}
