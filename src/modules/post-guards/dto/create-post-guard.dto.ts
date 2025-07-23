import { IsOptional, IsString, IsNumber, IsDate } from 'class-validator';
import { Type } from 'class-transformer';

export class CreatePostGuardDto {
  @IsOptional()
  @Type(() => Number)
  user_id?: number;

  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  postName?: string;

  @IsOptional()
  @IsString()
  typePos?: string;

  @IsOptional()
  @IsString()
  photo?: string;

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

  @IsOptional()
  @Type(() => Number)
  manual_positions_id?: number;
}
