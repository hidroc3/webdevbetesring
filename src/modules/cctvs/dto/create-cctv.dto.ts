import { IsNumber, IsOptional, IsString } from 'class-validator';

export class CreateCctvDto {
  @IsOptional() @IsString() name?: string;
  @IsOptional() @IsNumber() latitude?: number;
  @IsOptional() @IsNumber() longitude?: number;
  @IsOptional() @IsString() ip?: string;
  @IsOptional() @IsString() status?: string;
  @IsOptional() @IsString() username?: string;
  @IsOptional() @IsString() password?: string;
}
