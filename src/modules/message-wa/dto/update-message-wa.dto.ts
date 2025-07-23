import { PartialType } from '@nestjs/mapped-types';
import { CreateMessageWaDto } from './create-message-wa.dto';

export class UpdateMessageWaDto extends PartialType(CreateMessageWaDto) {}
