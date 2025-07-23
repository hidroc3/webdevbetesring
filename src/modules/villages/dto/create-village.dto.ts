import { IsString, IsNotEmpty, IsNumber } from 'class-validator';

export class CreateVillageDto {
  @IsNumber()
  id: number;

  @IsString()
  @IsNotEmpty()
  name: string;

  @IsNumber()
  subDistrictId: number;
}
