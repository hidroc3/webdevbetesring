import { PartialType } from '@nestjs/mapped-types';
import { CreateAwlrLogDto } from './create-awlr-log.dto';

export class UpdateAwlrLogDto extends PartialType(CreateAwlrLogDto) {}
