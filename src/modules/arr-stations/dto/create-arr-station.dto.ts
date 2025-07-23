import { IsString, IsOptional } from 'class-validator';

export class CreateArrStationDto {
  @IsString()
  post_name: string;

  @IsString()
  device_id: string;

  @IsOptional()
  @IsString()
  location?: string;
}
