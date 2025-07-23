import { PartialType } from '@nestjs/mapped-types';
import { CreatePostGuardDto } from './create-post-guard.dto';

export class UpdatePostGuardDto extends PartialType(CreatePostGuardDto) {}
