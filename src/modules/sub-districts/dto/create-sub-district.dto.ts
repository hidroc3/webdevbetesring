import { IsString, IsNotEmpty, IsNumber } from 'class-validator';

export class CreateSubDistrictDto {
  @IsNumber()
  id: number;

  @IsString()
  @IsNotEmpty()
  name: string;

  @IsNumber()
  cityId: number;
}
