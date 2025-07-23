import { IsOptional, IsNumber, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateArrLogDto {
  @IsOptional()
  @Type(() => Number)
  arr_station_id?: number;

  @IsOptional()
  @Type(() => Date)
  time?: Date;

  @IsOptional()
  @Type(() => Number)
  rainfall?: number;

  @IsOptional()
  @Type(() => Number)
  battery?: number;

  @IsOptional()
  @Type(() => Number)
  panel_temperature?: number;

  @IsOptional()
  @IsString()
  status?: string;
}
