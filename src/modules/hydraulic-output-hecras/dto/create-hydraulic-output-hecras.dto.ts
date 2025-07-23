import { IsNumber, IsOptional, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateHydraulicOutputHecrasDto {
  @IsOptional()
  @Type(() => Number)
  hydrologic_output_id?: number;

  @IsOptional()
  @Type(() => Date)
  time?: Date;

  @IsOptional()
  @Type(() => Number)
  flood_depth?: number;

  @IsOptional()
  @Type(() => Number)
  flow_velocity?: number;

  @IsOptional()
  @Type(() => Number)
  inundation_area?: number;

  @IsOptional()
  @IsString()
  model_type?: string;

  @IsOptional()
  @IsString()
  model_version?: string;

  @IsOptional()
  @IsString()
  parameter_set?: string;

  @IsOptional()
  @Type(() => Number)
  village_id?: number;
}
