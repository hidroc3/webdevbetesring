import { PartialType } from '@nestjs/mapped-types';
import { CreateCctvDto } from './create-cctv.dto';

export class UpdateCctvDto extends PartialType(CreateCctvDto) {}
