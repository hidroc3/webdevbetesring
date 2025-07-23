import { PartialType } from '@nestjs/mapped-types';
import { CreateArrLogDto } from './create-arr-log.dto';

export class UpdateArrLogDto extends PartialType(CreateArrLogDto) {}
