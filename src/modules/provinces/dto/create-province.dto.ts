import { IsString, IsNotEmpty, IsNumber } from 'class-validator';

export class CreateProvinceDto {
  @IsNumber()
  id: number;

  @IsString()
  @IsNotEmpty()
  name: string;
}
