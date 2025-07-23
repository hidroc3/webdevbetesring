import { PartialType } from '@nestjs/mapped-types';
import { CreateAwlrLogManualDto } from './create-awlr-log-manual.dto';

export class UpdateAwlrLogManualDto extends PartialType(
  CreateAwlrLogManualDto,
) {}
