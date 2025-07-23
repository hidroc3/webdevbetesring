import { IsString, IsNotEmpty, IsNumber } from 'class-validator';

export class CreateCityDto {
  @IsNumber()
  id: number;

  @IsString()
  @IsNotEmpty()
  name: string;

  @IsNumber()
  provinceId: number;
}
