import { IsNumber, IsOptional, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateAwsLogManualDto {
  @IsOptional()
  @Type(() => Number)
  user_id?: number;

  @IsOptional()
  @Type(() => Number)
  arr_id?: number;

  @IsOptional()
  @IsString()
  time?: string;

  @IsOptional()
  @Type(() => Date)
  date?: Date;

  @IsOptional()
  @Type(() => Number)
  value?: number;

  @IsOptional()
  @IsString()
  unit?: string;
}
