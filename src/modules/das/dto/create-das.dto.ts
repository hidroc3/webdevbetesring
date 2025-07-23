import { IsOptional, IsString } from 'class-validator';

export class CreateDasDto {
  @IsOptional()
  @IsString()
  name?: string;
}
