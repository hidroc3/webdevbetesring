import { IsDateString, IsNumber, IsOptional, IsString } from 'class-validator';

export class CreateAwsLogDto {
  @IsOptional()
  @IsNumber()
  aws_station_id?: number;

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
  @IsString()
  status?: string;
}
