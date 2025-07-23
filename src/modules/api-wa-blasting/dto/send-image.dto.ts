import { IsString, IsUrl, IsOptional, IsNotEmpty } from 'class-validator';

export class SendImageDto {
  @IsString()
  @IsNotEmpty()
  phoneNumber: string;

  @IsUrl()
  @IsNotEmpty()
  imageUrl: string;

  @IsString()
  @IsOptional()
  caption?: string;
}
