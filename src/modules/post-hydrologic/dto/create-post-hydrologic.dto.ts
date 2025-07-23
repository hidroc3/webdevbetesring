import { IsOptional, IsString } from 'class-validator';

export class CreatePostHydrologicDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  type_pos?: string;
}
