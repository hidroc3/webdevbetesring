import { PartialType } from '@nestjs/mapped-types';
import { CreateHydrologicOutputHechmsDto } from './create-hydrologic-output-hechms.dto';

export class UpdateHydrologicOutputHechmsDto extends PartialType(
  CreateHydrologicOutputHechmsDto,
) {}
