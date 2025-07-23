import { IsNumber, IsOptional, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateHydrologicOutputHechmsDto {
  @IsOptional()
  @Type(() => Number)
  das_id?: number;

  @IsOptional()
  @Type(() => Date)
  time?: Date;

  @IsOptional()
  @Type(() => Number)
  debit?: number;

  @IsOptional()
  @Type(() => Number)
  volume?: number;

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
  awlr_station_id?: number;

  @IsOptional()
  @Type(() => Number)
  aws_station_id?: number;

  @IsOptional()
  @Type(() => Number)
  arr_station_id?: number;
}
