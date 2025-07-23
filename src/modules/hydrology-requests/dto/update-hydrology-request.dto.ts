import { PartialType } from '@nestjs/mapped-types';
import { CreateHydrologyRequestDto } from './create-hydrology-request.dto';

export class UpdateHydrologyRequestDto extends PartialType(
  CreateHydrologyRequestDto,
) {}
