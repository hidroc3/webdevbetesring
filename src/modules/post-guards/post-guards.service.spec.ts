import { Test, TestingModule } from '@nestjs/testing';
import { PostGuardsService } from './post-guards.service';

describe('PostGuardsService', () => {
  let service: PostGuardsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PostGuardsService],
    }).compile();

    service = module.get<PostGuardsService>(PostGuardsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
