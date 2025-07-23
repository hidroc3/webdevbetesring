import { PartialType } from '@nestjs/mapped-types';
import { CreateHydraulicOutputHecrasDto } from './create-hydraulic-output-hecras.dto';

export class UpdateHydraulicOutputHecrasDto extends PartialType(
  CreateHydraulicOutputHecrasDto,
) {}
