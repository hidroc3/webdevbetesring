import { IsString, IsOptional } from 'class-validator';

export class CreateHydrologyRequestDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  email?: string;

  @IsOptional()
  @IsString()
  phone?: string;

  @IsOptional()
  @IsString()
  institution?: string;

  @IsOptional()
  @IsString()
  uploaded_letter?: string;

  @IsOptional()
  @IsString()
  feedback?: string;
}
