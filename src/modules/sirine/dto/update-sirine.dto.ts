import { PartialType } from '@nestjs/mapped-types';
import { CreateSirineDto } from './create-sirine.dto';

export class UpdateSirineDto extends PartialType(CreateSirineDto) {}
