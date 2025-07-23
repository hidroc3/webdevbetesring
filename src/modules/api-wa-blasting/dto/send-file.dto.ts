import { IsString, IsUrl, IsOptional, IsNotEmpty } from 'class-validator';

export class SendFileDto {
  @IsString()
  @IsNotEmpty()
  phoneNumber: string;

  @IsUrl()
  @IsNotEmpty()
  fileUrl: string;

  @IsString()
  @IsOptional()
  caption?: string;

  @IsString()
  @IsOptional()
  filename?: string;
}
