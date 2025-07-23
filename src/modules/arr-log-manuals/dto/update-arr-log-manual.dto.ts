import { PartialType } from '@nestjs/mapped-types';
import { CreateArrLogManualDto } from './create-arr-log-manual.dto';

export class UpdateArrLogManualDto extends PartialType(CreateArrLogManualDto) {}
