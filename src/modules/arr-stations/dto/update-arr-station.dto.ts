import { IsOptional, IsNumber, IsDate, IsString } from 'class-validator';

export class UpdateArrStationDto {
  @IsOptional()
  @IsDate()
  time?: Date;

  @IsOptional()
  @IsNumber()
  rainfall?: number;

  @IsOptional()
  @IsNumber()
  battery?: number;

  @IsOptional()
  @IsString()
  post_name?: string; // ✅ Tambahkan ini
}
