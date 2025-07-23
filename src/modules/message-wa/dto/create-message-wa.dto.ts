import { IsOptional, IsString, IsNumber, IsDate } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateMessageWaDto {
  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  contact_id?: number;

  @IsOptional()
  @IsString()
  messageText?: string;

  @IsOptional()
  @Type(() => Date)
  created_at?: Date;
}
