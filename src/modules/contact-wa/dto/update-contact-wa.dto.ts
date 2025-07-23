import { PartialType } from '@nestjs/mapped-types';
import { CreateContactWaDto } from './create-contact-wa.dto';

export class UpdateContactWaDto extends PartialType(CreateContactWaDto) {}
