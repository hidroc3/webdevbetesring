import { IsArray, ValidateNested, IsNumber, IsOptional } from 'class-validator';
import { Type } from 'class-transformer';

export class UpdateAwlrStationItemDto {
  @IsNumber()
  id: number;

  @IsOptional()
  @IsNumber()
  flow_a?: number;

  @IsOptional()
  @IsNumber()
  flow_b?: number;

  @IsOptional()
  @IsNumber()
  flow_ho?: number;
}

export class UpdateAwlrStationBulkDto {
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => UpdateAwlrStationItemDto)
  items: UpdateAwlrStationItemDto[];
}
