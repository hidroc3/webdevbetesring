import { IsNumber, IsOptional, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateAwlrLogDto {
  @IsNumber()
  @Type(() => Number)
  awlr_id: number;

  @IsString()
  time: string;

  @IsOptional()
  @Type(() => Number)
  water_level?: number;

  @IsOptional()
  @IsString()
  status?: string;
}
