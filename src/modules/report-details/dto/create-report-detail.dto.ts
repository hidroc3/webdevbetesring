import { IsOptional, IsString, IsNumber, IsDateString } from 'class-validator';

export class CreateReportDetailDto {
  @IsOptional()
  @IsNumber()
  category_id?: number;

  @IsOptional()
  @IsString()
  title?: string;

  @IsOptional()
  @IsString()
  news?: string;

  @IsOptional()
  @IsDateString()
  date?: string;

  @IsOptional()
  @IsString()
  status?: string;

  @IsOptional()
  @IsString()
  image?: string;
}
